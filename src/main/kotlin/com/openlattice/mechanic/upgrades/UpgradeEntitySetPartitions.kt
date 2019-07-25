package com.openlattice.mechanic.upgrades


import com.openlattice.edm.set.EntitySetFlag
import com.openlattice.mechanic.Toolbox
import com.openlattice.postgres.PostgresColumn.*
import com.openlattice.postgres.PostgresTable.ENTITY_KEY_IDS
import com.openlattice.postgres.PostgresTable.ENTITY_SETS
import com.openlattice.postgres.ResultSetAdapters
import com.openlattice.postgres.streams.BasePostgresIterable
import com.openlattice.postgres.streams.StatementHolderSupplier
import org.slf4j.LoggerFactory
import java.util.*

class UpgradeEntitySetPartitions(private val toolbox: Toolbox) : Upgrade {

    companion object {
        private const val PRIMEY_BOI = 257
        private val PARTITION_RANGE = 1..PRIMEY_BOI
        private val logger = LoggerFactory.getLogger(MigratePropertyValuesToDataTable::class.java)

    }

    private var THE_BIG_ONE = 0L

    override fun upgrade(): Boolean {
        //Manually ran upgrade to alter entity sets table.

        val entitySetSizes = retrieveEntitySetSizes()
        THE_BIG_ONE = entitySetSizes.values.maxBy { it.count }?.count ?: throw IllegalStateException(
                "Entity set sizes cannot be null."
        )

        entitySetSizes.forEach { (entitySetId, entitySetInfo) ->
            val partitions = getPartitions(entitySetId, entitySetInfo)
            val entitySet = toolbox.entitySets.getValue(entitySetId)
            if (entitySet.partitions.size > 0) {
                logger.error("SOMETHING BAD -- we came across entity set {} which has partitions assigned", entitySet.id)
            } else {
                entitySet.setPartitions(partitions)
                toolbox.esms.store(entitySetId, entitySet)
                logger.info(
                        "Partitions for entity set {} ({}) => ({},{})",
                        entitySet.name,
                        entitySet.id,
                        entitySet.partitions,
                        entitySet.partitionsVersion
                )
            }
        }

        return true
    }

    private fun retrieveEntitySetSizes(): Map<UUID, EntitySetInfo> {
        return BasePostgresIterable<Pair<UUID, EntitySetInfo>>(
                StatementHolderSupplier(toolbox.hds, GET_ENTITY_SET_COUNT)
        ) { rs ->
            ResultSetAdapters.entitySetId(rs) to EntitySetInfo(
                    ResultSetAdapters.count(rs),
                    ResultSetAdapters.entitySetFlags(rs)
            )
        }.toMap()
    }

    private fun getPartitions(entitySetId: UUID, entitySetInfo: EntitySetInfo): List<Int> {
        return if (entitySetInfo.flags.contains(EntitySetFlag.AUDIT)) {
            PARTITION_RANGE.toList()
        } else {
            when (entitySetId) {
                //NCRIC EntitySets
                UUID.fromString("65705aee-52af-4199-b013-83d8041af42d"),
                UUID.fromString("892b4c4f-578d-45e7-81e6-acbbf849031c"),
                UUID.fromString("4f4f94f1-5e04-425d-a22a-5f191671a817"),
                UUID.fromString("61939128-b38f-400a-8183-4fb6a0d84bc7"),
                UUID.fromString("2be6cd8c-a66e-4d45-9197-66ff91d99102"),
                UUID.fromString("21a4de85-0faa-4417-b073-270dd72f6384"),
                UUID.fromString("e3f97f98-a36f-470e-aa90-dbbcc02184ca"),
                UUID.fromString("38ee5039-cdf2-4158-9be9-5466c42d0345"),
                UUID.fromString("7416886d-ec88-4d42-b62d-322ad31dfe72"),
                UUID.fromString("12481c74-cfea-4b96-b132-dbbee3abb405"),
                UUID.fromString("81c7e707-f8bf-4685-8761-f81800c5d2f1"),
                UUID.fromString("c8a4d76e-8a5d-43c1-9f36-661b898036ed"),
                UUID.fromString("52ef171a-37fb-445b-9c00-8dd99d0e3649"),
                UUID.fromString("5bfa1659-a03e-4da8-8f34-8565c5f6550e"),
                UUID.fromString("ed316b25-e6de-4532-9043-e3f1c96982e3"),
                UUID.fromString("6bd62b44-4bab-4df1-b8c3-69d9f6f25380"),
                UUID.fromString("77a97eae-3a99-4b81-88a6-8ba2424cc936"),
                UUID.fromString("a223cfd7-0906-48f5-81d5-f03f1ace7c01"),
                UUID.fromString("b47a9f9a-60eb-42c1-9cb2-02957aaa80de") -> PARTITION_RANGE.toList()
                //Chronicle data sets
                UUID.fromString("fda9b1c1-6cea-4130-8d86-057e659bb9ea"),
                UUID.fromString("671ec7c3-99a1-423b-98ff-c7e2b3ea6efc"),
                UUID.fromString("d960f6c4-08b7-4d75-af18-396af170639d") -> PARTITION_RANGE.toList()
                //South Dakota entity sets (arbitrarily set to partition 24)
                UUID.fromString("fa8dd8b5-1c63-448d-874c-f0b95fd2d34c"),
                UUID.fromString("f64765c9-aab0-4132-ac19-054d79631245"),
                UUID.fromString("e4559b24-17bf-4c24-9d6c-9e218a1440ea"),
                UUID.fromString("aa08ab12-95cf-4246-b468-c3cfabd18b8e"),
                UUID.fromString("9de47145-aa7e-437e-8c55-25066486e889"),
                UUID.fromString("183df52c-99ec-4295-bc14-cf718fdae042"),
                UUID.fromString("aef2ecf8-6225-45ce-8bcf-0397b90712cd"),
                UUID.fromString("5511d8b0-de7b-4dd0-8d35-575a31c469b2"),
                UUID.fromString("abed0de0-e358-4539-859e-c5b63e371c6e"),
                UUID.fromString("9d86d75f-9615-42bd-a441-764b04324d06"),
                UUID.fromString("0a48710c-3899-4743-b1f7-28c6f99aa202"),
                UUID.fromString("d724d8f2-da4c-46e0-b5d8-5db8c3367b50"),
                UUID.fromString("ed5716db-830b-41b7-9905-24fa82761ace"),
                UUID.fromString("e46a4b2c-e609-4531-a7d4-a5274b2e39cd"),
                UUID.fromString("51097efb-1647-476b-80c7-b8c31c168bd7"),
                UUID.fromString("e9e38764-1f16-4b98-a173-2f0dd6ae9b8c"),
                UUID.fromString("c3a43642-b995-456f-879f-8b38ea2a2fc3"),
                UUID.fromString("d4e29d9c-df8c-4c30-a405-2e6941601fbc"),
                UUID.fromString("3a52bdec-5bb6-4809-84db-e1bdea66fa6b"),
                UUID.fromString("f1b446b5-622f-4815-9737-2932e831cafb"),
                UUID.fromString("ed7ec2c3-9c66-4155-b4d6-c8d2cdaa4296"),
                UUID.fromString("da9bcdf0-4fc8-4cde-9ebc-48133e5e9348"),
                UUID.fromString("c80aae3a-7d21-4a6e-9672-5adf34f65e1e"),
                UUID.fromString("f267f872-0fc6-4530-9156-08d57635d528"),
                UUID.fromString("8549f48b-8827-4bf3-b093-ab34a25c543c"),
                UUID.fromString("9f892773-7354-4802-b49d-d6b791e409e7"),
                UUID.fromString("d81fa49d-549b-4978-8fe7-1f568a909850"),
                UUID.fromString("8615d6ac-a1a1-42a5-a3c5-89b3121e8854"),
                UUID.fromString("68cd6423-d288-4018-9ec1-a5ae6787b101"),
                UUID.fromString("1b110888-0757-4d90-bd81-0fcaaf58de39"),
                UUID.fromString("c0de9cd2-8371-4780-9afd-779a22f13799"),
                UUID.fromString("72230e7a-9441-4938-86fa-1db2a80265d0"),
                UUID.fromString("26ecfb3d-db29-491f-ac10-e524a8588895"),
                UUID.fromString("066aab8f-1703-44e4-8dea-82bf88310d6b"),
                UUID.fromString("c674cecb-9977-4c26-bef9-6db481dde3ac"),
                UUID.fromString("c8dc1581-445e-47c0-a81f-70d8b6da5424"),
                UUID.fromString("c5da7a05-24a4-480e-9573-f5a118daec1a"),
                UUID.fromString("3a195330-fc5f-4be7-8d6c-53fd44ca10e6"),
                UUID.fromString("fa29142b-648c-4c62-a097-343802c3bf5d"),
                UUID.fromString("7b7b9bba-955c-46f7-b588-9fdde0248468"),
                UUID.fromString("a79a0fb6-181d-441d-81b3-040741b7ee8b"),
                UUID.fromString("52ad520d-5a98-4a0d-bcb2-eb12f2e05445"),
                UUID.fromString("73eec1ed-bf50-41a8-aa05-5866d6bc4e44"),
                UUID.fromString("f8f05c3c-d99b-4939-980b-e19df4ec63a5"),
                UUID.fromString("d2989930-8c4d-49aa-833f-56612977e839"),
                UUID.fromString("adc4db1e-a2ba-465d-a448-19ed1e74aa84"),
                UUID.fromString("35e34daf-2904-417b-81c3-23e55faabe3a"),
                UUID.fromString("32e44e1f-5587-4ef8-ad37-e93ef9bdcbb9"),
                UUID.fromString("399f142c-8e1c-4bf9-94e7-af4194db26c3"),
                UUID.fromString("7e9591f0-989f-4b6e-9782-6141f54171b8"),
                UUID.fromString("33c594b5-ce87-451b-b495-0f1612f7966c"),
                UUID.fromString("8d493e9f-9e15-4941-9ad0-65380b6f5af3"),
                UUID.fromString("f25af9ae-4e95-45cc-a0d4-99d4de7db293"),
                UUID.fromString("1866931f-438f-4921-a0bf-c3d3918ca1e1"),
                UUID.fromString("b66606a0-f8b1-4023-aa22-ead7f571ba3b"),
                UUID.fromString("8b2b7dc2-cb89-491a-b42c-791a8f568495"),
                UUID.fromString("20019b0a-23e7-444e-847d-2dc282bf9b5b"),
                UUID.fromString("1ef126df-e94f-41ae-8a6b-43942758b933"),
                UUID.fromString("85e350d1-9e96-4308-b5ec-80fee1a1caa1"),
                UUID.fromString("f68439e7-fb47-4b9a-b594-687d62dbd32c"),
                UUID.fromString("70807432-e133-4a7b-a35e-50998298f987"),
                UUID.fromString("8c9237fb-0319-4c08-9ab6-29a7525de1c9"),
                UUID.fromString("1a4d5164-2bc1-4713-8e46-83bed2e13ca3"),
                UUID.fromString("cd47f63a-2b30-4254-b3a6-d77af96d6569"),
                UUID.fromString("afd7a637-88ea-4a77-82c9-80324635357e"),
                UUID.fromString("03b9752a-6bd1-44f1-b429-250a446e5643"),
                UUID.fromString("b5806572-3c3e-469e-8010-0433097a1d77"),
                UUID.fromString("93cbda53-5bef-43d2-8605-52621aeae48a"),
                UUID.fromString("a1648fd0-0274-4fc2-8f36-d4428c4455b6"),
                UUID.fromString("aadde33e-d17f-45f1-a966-4d16d033a8a0"),
                UUID.fromString("64cc0ba6-36f5-489d-b1f6-99f53d71a80f"),
                UUID.fromString("b6015703-278d-4595-ae88-939da0b91816"),
                UUID.fromString("c7811a15-d774-4417-a9cc-52f7e28672b5"),
                UUID.fromString("52e9e9b8-5bcf-4015-ba97-3c2256a334f1"),
                UUID.fromString("88c3568a-2c2f-4e7a-8987-38c8b36cc164"),
                UUID.fromString("35c12a2e-4984-4423-b02a-211a59a88d12"),
                UUID.fromString("7a1bd40d-1a31-4c2a-ade3-fa16eaa9f56b"),
                UUID.fromString("9e7eb62f-4170-417f-b922-93ad8af8c2d9"),
                UUID.fromString("d09b2570-efb6-4ecd-9fde-dc122c97e6ac"),
                UUID.fromString("d6d6eea1-f2d3-4776-8583-8da7a2cfdb0e"),
                UUID.fromString("4f365c13-0640-4da8-bea5-c1c5e3a9f7af"),
                UUID.fromString("c9ab56f1-5204-4b01-afde-70afaaee9155"),
                UUID.fromString("fdbc7dc9-9f5e-4438-8837-bb969cbdf4d0"),
                UUID.fromString("14d5501a-a85a-4c59-bf54-460633159709"),
                UUID.fromString("ce92a664-6b15-438e-9fca-594f1df61227"),
                UUID.fromString("7a6fb1d7-6b68-48a2-8639-3d2d53f7437c"),
                UUID.fromString("8a4547e4-48cf-477c-be0a-15415c56fe10"),
                UUID.fromString("2e75dd35-7e23-42b8-84b1-3ba3aa343e94"),
                UUID.fromString("2dcd95d7-5f50-491d-9604-83ac0c217102"),
                UUID.fromString("9d5cf169-f7da-43c7-a7ee-93e640d7d663"),
                UUID.fromString("ca8eb35b-d597-455b-82ae-36eee3b8e0ff"),
                UUID.fromString("e92f5e36-ad8c-4e74-8fb5-b1c233d995d3"),
                UUID.fromString("f5044fcd-5ae5-419c-81c3-3a1c16272302"),
                UUID.fromString("e2cee1bf-bde0-455e-93b6-6b3f94fd781f"),
                UUID.fromString("59bda798-147b-47f2-b898-2e9f50819fe1"),
                UUID.fromString("c40aad67-8347-4e42-9485-ead842e7b28f"),
                UUID.fromString("0f8c334b-b4bb-4073-84d7-4772f8f7748d"),
                UUID.fromString("48d4d37d-8969-4fe8-bb90-e5d69eef20b1"),
                UUID.fromString("5ea29b47-6fde-4156-b8ec-1208e6ee4f50"),
                UUID.fromString("0241169a-55ae-49d8-89c7-6d74848ae7d4"),
                UUID.fromString("a0320fed-fa85-4f34-82f9-588033021ca4"),
                UUID.fromString("24aa8185-ffde-4aac-b555-a88b5c2e1999"),
                UUID.fromString("6b5dada9-f8f5-4442-abf0-fb8da6d505ba"),
                UUID.fromString("fe189fad-125c-4c79-a67d-76a3533c41f2"),
                UUID.fromString("45be739a-c87b-4c3f-8900-327a2b574733"),
                UUID.fromString("e4574e58-8609-408b-92e7-b2a509024779"),
                UUID.fromString("81f72840-25f7-4e4f-b03f-595f86555661"),
                UUID.fromString("745e18be-7b7c-4c19-951e-5e79808178f9"),
                UUID.fromString("7cbbb1ce-691f-41d7-bfe0-56a898520623"),
                UUID.fromString("bab936f2-4833-4f58-9298-23ba1ae35214"),
                UUID.fromString("6bb61e76-c601-47a3-a986-af86c6c1bf83"),
                UUID.fromString("fb3ce259-e4ab-4346-93ff-fbb459cda47c"),
                UUID.fromString("d16ad959-5f37-4d37-9a80-e114a05e690d"),
                UUID.fromString("d57f8ff6-f632-44ca-86cb-66d6007d7cf8"),
                UUID.fromString("0ad8ad30-75bb-4109-a9f9-924bcc0bf84f"),
                UUID.fromString("f770521b-af16-478e-9000-a98c2628009d"),
                UUID.fromString("a825616a-9b83-4f52-a31e-94c42e6d4c71"),
                UUID.fromString("56266017-b8b2-4630-8e93-91013ac614a6"),
                UUID.fromString("2c38cfb6-6422-404a-86bf-d26d24a52d8e"),
                UUID.fromString("cb519a49-4747-419e-bb81-c37f17227020"),
                UUID.fromString("626ade80-8bc2-4137-ae1b-c78967a9191c"),
                UUID.fromString("a583d9e1-844c-4ad1-8bd4-e8cf82863132"),
                UUID.fromString("d6cac010-5680-44f9-824b-98c4e2550f2f"),
                UUID.fromString("278dcead-2829-4850-8c0a-60650b0f71d6"),
                UUID.fromString("440e2fe5-4adb-42b5-86cf-55598038e0aa"),
                UUID.fromString("1c9fcca3-44f7-413a-a91f-ed52ec48d773"),
                UUID.fromString("86e32421-15e4-457b-9024-0cb4a1eab976"),
                UUID.fromString("50072f07-b1a3-49fb-9ffe-682a6630918b"),
                UUID.fromString("5917fe56-3f97-47f0-b87c-5e22450e097f"),
                UUID.fromString("c61db18a-454c-44cb-b394-3dea73896030"),
                UUID.fromString("7db7fb83-0291-4dfa-bcaa-295e81c90853"),
                UUID.fromString("31c3d8e1-e1e1-4911-a00e-f38a534f045a"),
                UUID.fromString("e62490fc-a53e-4181-a45b-1519fdc2e68d"),
                UUID.fromString("ee2fc738-38f6-4655-bc79-5e87a6f85501"),
                UUID.fromString("1b1cd21f-ca69-4fda-981e-230695676710"),
                UUID.fromString("9d38aa5a-2c83-4afa-839c-1e03089fec30"),
                UUID.fromString("6bdc14c6-5a9d-4dbd-8c84-5331e59925d3"),
                UUID.fromString("9802bc1a-c024-4fea-88d4-d7906fdf2ca3"),
                UUID.fromString("6010b530-7243-4de6-95f9-8dbc59e34872"),
                UUID.fromString("0ac30441-caac-4949-b835-f37b9e19a3ca"),
                UUID.fromString("7c60ed45-95fc-4347-b6e3-2077ff906812"),
                UUID.fromString("3fa68a17-a8ff-4210-b6e0-6b53872a82a5"),
                UUID.fromString("851ffa0b-aa25-43c3-b04e-ddd5b34001e4"),
                UUID.fromString("e723c930-02e1-47da-bc06-84db06fc2875"),
                UUID.fromString("76c28062-a7ba-42c9-b1e3-c943420ce0ce"),
                UUID.fromString("5f382d7a-6937-44de-834d-93c9834c23ab"),
                UUID.fromString("fdec4c8e-4086-4b21-8c2f-b14ac7269ba7"),
                UUID.fromString("8f5d484c-605d-49b1-b1d5-a0b7c3b762d0"),
                UUID.fromString("3b512a00-3652-4890-9877-38dc090df6c1"),
                UUID.fromString("12364dea-8b37-4c7e-acfc-100ceb2d66fc"),
                UUID.fromString("b1aa1e81-c13a-4fb1-afeb-4b3965122a02"),
                UUID.fromString("322e1887-4e79-4c1f-b137-2dfb8c19d1d4"),
                UUID.fromString("194e534b-fb70-471a-8635-d4862e41eaba"),
                UUID.fromString("13fabe81-df3e-40d1-bc18-82416af4cc0f"),
                UUID.fromString("ad66a34f-436f-46c8-8a3c-669b171cf248"),
                UUID.fromString("5e3d0e47-f696-4304-bdaf-ae6d97506480"),
                UUID.fromString("57e8502d-d0bc-4283-9e1c-9f69b59547dd"),
                UUID.fromString("ca47b651-69b3-4e73-904b-0319b5f79b92"),
                UUID.fromString("3208a67e-5bfb-4c85-b8e9-8f27fd69dcc1"),
                UUID.fromString("fe8f0de7-6465-4f83-b571-777567f16323"),
                UUID.fromString("0bf1fb62-cfbf-4772-ab99-38765abdf3a3"),
                UUID.fromString("69af9e1e-3325-46f1-9a7a-6548f7f95c6a"),
                    // Lincoln
                UUID.fromString("19694e91-cbb5-4986-8d29-39654e63326f"),
                UUID.fromString("6389259b-8d91-4c1d-886a-868a38bb0f38"),
                UUID.fromString("c80aae3a-7d21-4a6e-9672-5adf34f65e1e"),
                UUID.fromString("52f0edc7-adc6-4ca7-b19b-7a1d9abe07ee"),
                UUID.fromString("6d991ee7-530c-4291-b089-787f8ed3ebd9"),
                UUID.fromString("183df52c-99ec-4295-bc14-cf718fdae042"),
                UUID.fromString("9d1830da-9b97-4d9c-afdc-289429963f55"),
                UUID.fromString("c0acc5b7-c047-48b0-ba29-ba626290618f"),
                UUID.fromString("5b258e60-45c5-4d54-9425-adbbcb532611"),
                UUID.fromString("32e44e1f-5587-4ef8-ad37-e93ef9bdcbb9"),
                UUID.fromString("c674cecb-9977-4c26-bef9-6db481dde3ac"),
                UUID.fromString("a5a6f444-3556-4445-9269-ddafe13e38be"),
                UUID.fromString("ef373fb5-ead5-48a5-8573-59469ef9dc22"),
                UUID.fromString("7e9591f0-989f-4b6e-9782-6141f54171b8"),
                UUID.fromString("b1d66212-f94d-44b6-932c-0f401d9dd285"),
                UUID.fromString("25de357e-9f4e-47de-aaaf-f8d5b2b3702a"),
                UUID.fromString("c3a43642-b995-456f-879f-8b38ea2a2fc3"),
                UUID.fromString("8ebf299f-52e8-43a0-b24e-977528e05b1f"),
                UUID.fromString("52ad520d-5a98-4a0d-bcb2-eb12f2e05445"),
                UUID.fromString("72230e7a-9441-4938-86fa-1db2a80265d0"),
                UUID.fromString("da9bcdf0-4fc8-4cde-9ebc-48133e5e9348"),
                UUID.fromString("35e34daf-2904-417b-81c3-23e55faabe3a"),
                UUID.fromString("e4ce8a79-7332-4d17-a6e5-560d4fecaa50"),
                UUID.fromString("ed5716db-830b-41b7-9905-24fa82761ace"),
                UUID.fromString("8f0e078d-9b34-41ea-9e3c-80bbd0300384"),
                UUID.fromString("84593a72-658d-4e34-beed-47142064607a"),
                UUID.fromString("9de47145-aa7e-437e-8c55-25066486e889"),
                UUID.fromString("adc4db1e-a2ba-465d-a448-19ed1e74aa84"),
                UUID.fromString("0eddb5ec-0110-4b25-9027-19dbe85c8451"),
                UUID.fromString("0a48710c-3899-4743-b1f7-28c6f99aa202"),
                UUID.fromString("a79a0fb6-181d-441d-81b3-040741b7ee8b"),
                UUID.fromString("86c5ddaf-b247-43e5-9b97-0d9b9d484ceb"),
                UUID.fromString("8549f48b-8827-4bf3-b093-ab34a25c543c"),
                UUID.fromString("bfe6ed29-d02d-4b9b-a5b7-278704f1d314"),
                UUID.fromString("7313acfe-91d8-4090-bf99-5aa60e92adba"),
                UUID.fromString("d724d8f2-da4c-46e0-b5d8-5db8c3367b50"),
                UUID.fromString("3aa11ece-b76b-402e-8f87-229c9ce89ef0"),
                UUID.fromString("7102a7f0-e833-4972-84ec-58656bbf8bba"),
                UUID.fromString("abed0de0-e358-4539-859e-c5b63e371c6e"),
                UUID.fromString("1bb999f1-11d2-4642-96b7-a5fe781520d0"),
                UUID.fromString("35f76525-dcf7-4de7-be5d-d735f36783d2"),
                UUID.fromString("066aab8f-1703-44e4-8dea-82bf88310d6b"),
                UUID.fromString("f4b2034a-5e8b-4b93-bd18-5661af546107"),
                UUID.fromString("fa29142b-648c-4c62-a097-343802c3bf5d"),
                UUID.fromString("fa8dd8b5-1c63-448d-874c-f0b95fd2d34c"),
                UUID.fromString("51097efb-1647-476b-80c7-b8c31c168bd7"),
                UUID.fromString("3721e041-faff-4c53-a26c-126c21c93288"),
                UUID.fromString("fbbc8c0b-676e-4723-a067-f5e9c68cdb5f"),
                UUID.fromString("e9e38764-1f16-4b98-a173-2f0dd6ae9b8c"),
                UUID.fromString("d81fa49d-549b-4978-8fe7-1f568a909850"),
                UUID.fromString("f06f9886-caa0-47e1-ae4a-9599dffd06f7"),
                UUID.fromString("399f142c-8e1c-4bf9-94e7-af4194db26c3"),
                UUID.fromString("68cd6423-d288-4018-9ec1-a5ae6787b101"),
                UUID.fromString("d2989930-8c4d-49aa-833f-56612977e839"),
                UUID.fromString("2b63cf00-20f2-42ea-9e99-b0e26d9df48f"),
                UUID.fromString("2bc7973b-1f01-438b-9dd3-c9e691a364c2"),
                UUID.fromString("0d946e50-2677-4a18-b80b-8e4afe354415"),
                UUID.fromString("92791a05-7441-4f2d-9d07-a334969bad39"),
                UUID.fromString("3b61121a-f752-4877-8524-1c282a92e067") -> listOf(24)
                else -> getPartitionValues(entitySetInfo.count)
            }
        }
    }

    private fun getPartitionValues(esSize: Long): List<Int> {
        val spread = Math.ceil(esSize.toDouble() / THE_BIG_ONE).toInt()
        var shuffled = PARTITION_RANGE.shuffled()
        return shuffled.take(spread)
    }

    override fun getSupportedVersion(): Long {
        return Version.V2019_07_01.value
    }
}

private data class EntitySetInfo(val count: Long, val flags: Set<EntitySetFlag>)

private val SELECT_ENTITY_SET_COUNTS = "(SELECT ${ENTITY_SET_ID.name}, count(*) FROM ${ENTITY_KEY_IDS.name} GROUP BY ${ENTITY_SET_ID.name}) as entity_set_counts"
private val SELECT_ENTITY_SET_FLAGS = "(select id as ${ENTITY_SET_ID.name}, ${FLAGS.name} from ${ENTITY_SETS.name} WHERE ${PARTITIONS.name} = '{}') as entity_set_flags"

private val GET_ENTITY_SET_COUNT = "SELECT ${ENTITY_SET_ID.name}, ${FLAGS.name}, CASE WHEN count IS NULL THEN 1 ELSE count END " +
        "FROM $SELECT_ENTITY_SET_FLAGS LEFT JOIN $SELECT_ENTITY_SET_COUNTS USING (entity_set_id) "